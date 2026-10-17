package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;

/**
 * W8 regression: the pending-match key must outlive its confirmation deadline.
 * If its TTL equals the deadline, the key expires before the sweeper reads it,
 * the match is never dissolved and the tickets stay in CONFIRMING forever.
 */
class PendingMatchTtlTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String QUEUE_KEY = "queue:1v1:NA";
    private static final String MATCH_ID = "match-ttl-test";

    @Test
    void pendingKeyTtlIsLongerThanTheConfirmationDeadline() {
        assertTrue(MatchmakingEngineApplication.PENDING_KEY_TTL_SECONDS
                        > MatchmakingEngineApplication.CONFIRMATION_TIMEOUT_SECONDS,
                "the pending key must outlive the confirmation deadline so the sweep can requeue the tickets");
    }

    @Test
    void pairScriptStoresThePendingKeyWithTheConfiguredTtl() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            String script = new String(getClass().getResourceAsStream("/pair.lua").readAllBytes(),
                    StandardCharsets.UTF_8);
            String sha = redis.send(Request.cmd(Command.SCRIPT).arg("LOAD").arg(script))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS).toString();
            redis.send(Request.cmd(Command.DEL).arg(QUEUE_KEY).arg("match-pending:" + MATCH_ID))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            redis.send(Request.cmd(Command.ZADD).arg(QUEUE_KEY).arg("1000").arg("ticket-ttl-a"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            redis.send(Request.cmd(Command.ZADD).arg(QUEUE_KEY).arg("2000").arg("ticket-ttl-b"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            Object result = redis.send(Request.cmd(Command.EVALSHA).arg(sha)
                            .arg("1").arg(QUEUE_KEY)
                            .arg(MATCH_ID).arg("{\"players\":[],\"expiresAt\":\"2030-01-01T00:00:00Z\"}")
                            .arg(String.valueOf(MatchmakingEngineApplication.PENDING_KEY_TTL_SECONDS))
                            .arg("2"))
                    .toCompletionStage().toCompletableFuture().get(15, TimeUnit.SECONDS);
            assertNotNull(result, "the pair script must win the race");

            long ttl = redis.send(Request.cmd(Command.TTL).arg("match-pending:" + MATCH_ID))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS).toLong();
            assertEquals(MatchmakingEngineApplication.PENDING_KEY_TTL_SECONDS, ttl,
                    "the pair script must store the pending key with the configured TTL");
        } finally {
            redis.send(Request.cmd(Command.DEL).arg(QUEUE_KEY).arg("match-pending:" + MATCH_ID))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
}
