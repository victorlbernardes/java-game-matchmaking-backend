package com.victor.matchmaking.api;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.api.infrastructure.RedisPlayerPresence;
import com.victor.matchmaking.redis.RedisKeys;

import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;

/**
 * Presence names the instance holding the socket, and only the current owner may clear it
 * (W19, ADR 0026).
 *
 * <p>The race this pins is the ordinary reconnect, not an exotic one: the client opens a socket on
 * instance B before instance A's close handler runs. With a plain {@code DEL} on disconnect, A's
 * late handler erases B's entry and presence points at the instance the player just left. The Lua
 * compare-and-clear turns that into a no-op, and this test fails if it is replaced.
 */
class PresenceReleaseTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String PLAYER = "presence-test-player";

    @Test
    void aLateDisconnectMustNotEraseTheNewInstancePresence() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            RedisPlayerPresence presence = new RedisPlayerPresence(redis);
            del(redis);

            presence.markOnline(PLAYER, "api-A").toCompletionStage().toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
            assertEquals("api-A", get(redis));
            long ttl = pttl(redis);
            assertTrue(ttl > 0 && ttl <= 90_000,
                    "presence must expire on its own (one and a half heartbeats), ttl=" + ttl);

            // The player reconnects on B before A notices the socket is gone.
            presence.markOnline(PLAYER, "api-B").toCompletionStage().toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
            presence.offline(PLAYER, "api-A").toCompletionStage().toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
            assertEquals("api-B", get(redis),
                    "the old instance's disconnect must not erase the new instance's presence");

            presence.offline(PLAYER, "api-B").toCompletionStage().toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
            assertNull(get(redis), "the owner's own disconnect clears it");
        } finally {
            del(redis);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static String get(Redis redis) throws Exception {
        Response response = redis.send(Request.cmd(Command.GET).arg(RedisKeys.presenceKey(PLAYER)))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return response != null ? response.toString() : null;
    }

    private static long pttl(Redis redis) throws Exception {
        Response response = redis.send(Request.cmd(Command.PTTL).arg(RedisKeys.presenceKey(PLAYER)))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return response.toLong();
    }

    private static void del(Redis redis) throws Exception {
        redis.send(Request.cmd(Command.DEL).arg(RedisKeys.presenceKey(PLAYER)))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
}
