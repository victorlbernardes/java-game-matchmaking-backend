package com.victor.matchmaking.api.infrastructure;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.redis.RedisKeys;

import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;

/**
 * Property: the number the placement trusts is the server's, it can only improve, and one nonce is
 * one measurement (W21, ADR 0039).
 *
 * <p>Three things have to be true at once, or the whole "measured, not declared" claim collapses:
 *
 * <ol>
 *   <li>The rtt is <b>timed by this server</b> between its own two requests - the client cannot
 *       report it, only delay it.</li>
 *   <li>A <b>bad sample never raises the profile</b>: one probe answered while the machine was busy
 *       would otherwise lock a player out of a region it reaches perfectly well, and the exit would
 *       be "wait five minutes".</li>
 *   <li>A nonce is <b>single use and bound</b> to the player and region that asked for it, so one
 *       round trip cannot be reported as several and one player cannot spend another's probe.</li>
 * </ol>
 */
class RedisPlayerLatencyTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String PLAYER = "w21-latency-player";
    private static final String OTHER_PLAYER = "w21-latency-other";
    private static final Region REGION = Region.NA;

    @Test
    void theServerTimesTheProbeAndStoresIt() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            RedisPlayerLatency latency = new RedisPlayerLatency(redis);
            clean(redis);

            PlayerLatencyProbe probe = issue(latency);
            Thread.sleep(40);
            Integer rttMs = await(latency.complete(PLAYER, REGION, probe.nonce()));

            assertNotNull(rttMs, "the pong closes the probe the server opened");
            assertTrue(rttMs >= 35, "the measurement must include the 40 ms the client waited, got " + rttMs);
            assertEquals(rttMs, await(latency.rtt(PLAYER, REGION)), "the profile holds what was measured");
            assertTrue(ttlMillis(redis) > 0, "the profile must expire on its own");
        } finally {
            clean(redis);
            redis.close();
            vertx.close();
        }
    }

    @Test
    void aNonceCannotBeAnsweredTwice() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            RedisPlayerLatency latency = new RedisPlayerLatency(redis);
            clean(redis);

            String nonce = issue(latency).nonce();
            assertNotNull(await(latency.complete(PLAYER, REGION, nonce)));
            assertNull(await(latency.complete(PLAYER, REGION, nonce)),
                    "a replayed pong must not be timed again");
            assertNull(await(latency.complete(PLAYER, REGION, "no-such-nonce")));
        } finally {
            clean(redis);
            redis.close();
            vertx.close();
        }
    }

    @Test
    void aWorseSampleNeverRaisesTheProfile() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            RedisPlayerLatency latency = new RedisPlayerLatency(redis);
            clean(redis);

            // Warm-up: the first probe of a fresh instance also pays for loading the two scripts,
            // and a measurement that includes a one-off cost is not a sample of the network.
            await(latency.complete(PLAYER, REGION, issue(latency).nonce()));
            // ...and then forget it. The profile keeps the *minimum*, so a warm-up that happened to
            // be the fastest sample would stay the answer and the assertion below would be about the
            // script cache, not about the sample this test took (observed: warm-up 7 ms, good 8 ms).
            clean(redis);

            // A good sample (answered immediately), then a bad one (answered after 150 ms).
            Integer good = await(latency.complete(PLAYER, REGION, issue(latency).nonce()));
            String slowNonce = issue(latency).nonce();
            Thread.sleep(150);
            Integer bad = await(latency.complete(PLAYER, REGION, slowNonce));

            assertNotNull(good);
            assertNotNull(bad);
            assertTrue(bad > good + 100, "the second probe must be the worse sample: " + bad + " vs " + good);
            assertEquals(good, await(latency.rtt(PLAYER, REGION)),
                    "a hiccup costs one probe, not the player's session");
        } finally {
            clean(redis);
            redis.close();
            vertx.close();
        }
    }

    @Test
    void aProbeIsBoundToItsPlayerAndRegion() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            RedisPlayerLatency latency = new RedisPlayerLatency(redis);
            clean(redis);

            String nonce = issue(latency).nonce();
            assertNull(await(latency.complete(OTHER_PLAYER, REGION, nonce)),
                    "another player must not be able to spend this probe");
            assertNull(await(latency.complete(PLAYER, Region.EU, nonce)),
                    "a pong answered at another region's endpoint is not this region's measurement");
            assertNotNull(await(latency.complete(PLAYER, REGION, nonce)),
                    "the refusal must not consume the nonce");
        } finally {
            clean(redis);
            redis.close();
            vertx.close();
        }
    }

    @Test
    void aMeasurementOnlyExistsInTheRegionThatTookIt() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            RedisPlayerLatency latency = new RedisPlayerLatency(redis);
            clean(redis);

            await(latency.complete(PLAYER, REGION, issue(latency).nonce()));
            assertNotNull(await(latency.rtt(PLAYER, REGION)));
            assertNull(await(latency.rtt(PLAYER, Region.EU)),
                    "measuring NA says nothing about EU, and must not be read as if it did");
            assertNull(await(latency.rtt(OTHER_PLAYER, REGION)));
        } finally {
            clean(redis);
            redis.close();
            vertx.close();
        }
    }

    private static PlayerLatencyProbe issue(RedisPlayerLatency latency) throws Exception {
        com.victor.matchmaking.api.application.PlayerLatency.Probe probe = await(latency.issue(PLAYER, REGION));
        assertNotNull(probe.nonce());
        assertEquals(REGION, probe.region());
        return new PlayerLatencyProbe(probe.nonce());
    }

    private record PlayerLatencyProbe(String nonce) {
    }

    private static long ttlMillis(Redis redis) throws Exception {
        io.vertx.redis.client.Response ttl = redis.send(Request.cmd(Command.PTTL)
                        .arg(RedisKeys.playerLatencyKey(PLAYER)))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return ttl != null ? ttl.toLong() : -2;
    }

    private static void clean(Redis redis) throws Exception {
        redis.send(Request.cmd(Command.DEL)
                        .arg(RedisKeys.playerLatencyKey(PLAYER))
                        .arg(RedisKeys.playerLatencyKey(OTHER_PLAYER)))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static <T> T await(io.vertx.core.Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }
}
