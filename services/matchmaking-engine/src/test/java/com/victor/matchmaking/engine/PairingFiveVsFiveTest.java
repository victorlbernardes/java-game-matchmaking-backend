package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;

/**
 * W9 test: a 5v5 queue must be consumed by exactly ten tickets.
 * The Lua script receives the player count and atomically removes that many
 * entries, creating one pending match with all ten players.
 */
class PairingFiveVsFiveTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String QUEUE_KEY = "queue:5v5:NA";
    private static final int PLAYER_COUNT = 10;

    @Test
    void fiveVsFiveQueuePairsTenPlayers() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            String script = new String(getClass().getResourceAsStream("/pair.lua").readAllBytes(), StandardCharsets.UTF_8);
            String sha = redis.send(Request.cmd(Command.SCRIPT).arg("LOAD").arg(script))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS).toString();
            assertNotNull(sha, "pair.lua must load");

            redis.send(Request.cmd(Command.DEL).arg(QUEUE_KEY).arg("match-pending:five-vs-five-1"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            // Queue ten tickets of the 5v5 mode.
            for (int i = 0; i < PLAYER_COUNT; i++) {
                String ticketId = String.format("ticket-five-vs-five-%02d", i);
                redis.send(Request.cmd(Command.SET).arg("ticket:" + ticketId)
                                .arg("{\"matchId\":\"" + ticketId + "\"}").arg("EX").arg("900"))
                        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
                redis.send(Request.cmd(Command.ZADD).arg(QUEUE_KEY).arg(String.valueOf(i))
                                .arg(ticketId))
                        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }

            // Pairing consumes exactly the player count.
            String pendingJson = pendingJsonFor(PLAYER_COUNT);
            Object result = redis.send(Request.cmd(Command.EVALSHA).arg(sha)
                            .arg("1").arg(QUEUE_KEY)
                            .arg("match-five-vs-five-1").arg(pendingJson).arg("30").arg(String.valueOf(PLAYER_COUNT)))
                    .toCompletionStage().toCompletableFuture().get(15, TimeUnit.SECONDS);
            assertNotNull(result, "pairing the ten tickets must succeed");
            assertEquals("match-five-vs-five-1", result.toString());

            // The pending match holds every player.
            io.vertx.redis.client.Response pending = redis.send(Request.cmd(Command.GET)
                            .arg("match-pending:match-five-vs-five-1"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertNotNull(pending, "the pending match must exist");
            assertEquals(PLAYER_COUNT, countPlayers(pending.toString()), "the pending match must hold ten players");

            // All ten tickets were removed from the queue.
            io.vertx.redis.client.Response queueSize = redis.send(Request.cmd(Command.ZCARD).arg(QUEUE_KEY))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(0L, queueSize.toLong(), "the queue must be empty after pairing");
        } finally {
            redis.send(Request.cmd(Command.DEL).arg(QUEUE_KEY)
                            .arg("match-pending:match-five-vs-five-1"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static String pendingJsonFor(int playerCount) {
        StringBuilder json = new StringBuilder("{\"players\":[");
        for (int i = 0; i < playerCount; i++) {
            if (i > 0) {
                json.append(',');
            }
            json.append("{\"playerId\":\"p").append(i).append("\",\"ticketId\":\"ticket-five-vs-five-")
                    .append(String.format("%02d", i)).append("\"}");
        }
        return json.append("],\"confirmations\":[],\"expiresAt\":\"2030-01-01T00:00:00Z\"}").toString();
    }

    private static int countPlayers(String pendingJson) {
        io.vertx.core.json.JsonObject pending = new io.vertx.core.json.JsonObject(pendingJson);
        return pending.getJsonArray("players").size();
    }
}
