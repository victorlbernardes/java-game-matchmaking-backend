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
 * Race-condition test: two workers pairing the same two tickets.
 * The atomic Lua script guarantees that only ONE worker wins — the second gets nil
 * because the first already removed the tickets from the queue (ZREM).
 *
 * If the script is removed (falls back to non-atomic ZRANGE+save), BOTH workers
 * would create a pending match for the same tickets, violating 1 ticket → 1 match.
 */
class PairingRaceTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");

    @Test
    void onlyOneWorkerWinsTheRace() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        String queueKey = "queue:1v1:NA";
        String ticketA = "ticket-race-a";
        String ticketB = "ticket-race-b";
        try {
            // Setup: load script, create 2 tickets in the queue
            String script = new String(getClass().getResourceAsStream("/pair.lua").readAllBytes(), StandardCharsets.UTF_8);
            String sha = redis.send(Request.cmd(Command.SCRIPT).arg("LOAD").arg(script))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS).toString();
            assertNotNull(sha, "pair.lua must load");

            redis.send(Request.cmd(Command.DEL).arg(queueKey).arg("match-pending:race-1").arg("match-pending:race-2"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            redis.send(Request.cmd(Command.ZADD).arg(queueKey).arg("1000").arg(ticketA))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            redis.send(Request.cmd(Command.ZADD).arg(queueKey).arg("2000").arg(ticketB))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            // Worker 1 pairs the 2 tickets (atomic ZREM + SET)
            Object result1 = redis.send(Request.cmd(Command.EVALSHA).arg(sha)
                            .arg("1").arg(queueKey)
                            .arg("match-race-1").arg("{\"players\":[],\"expiresAt\":\"2030-01-01T00:00:00Z\"}").arg("30").arg("2"))
                    .toCompletionStage().toCompletableFuture().get(15, TimeUnit.SECONDS);
            assertNotNull(result1, "worker1 must return a result");
            assertEquals("match-race-1", result1.toString(), "worker1 must win the race");

            // Worker 2 tries to pair the same 2 tickets — but they are already removed from the queue
            Object result2 = redis.send(Request.cmd(Command.EVALSHA).arg(sha)
                            .arg("1").arg(queueKey)
                            .arg("match-race-2").arg("{\"players\":[],\"expiresAt\":\"2030-01-01T00:00:00Z\"}").arg("30").arg("2"))
                    .toCompletionStage().toCompletableFuture().get(15, TimeUnit.SECONDS);
            assertNull(result2, "worker2 must lose the race (tickets already removed by worker1)");

            // The winning worker removed both tickets from the queue
            io.vertx.redis.client.Response remaining = redis.send(Request.cmd(Command.ZCARD).arg(queueKey))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            long remainingCount = remaining != null ? remaining.toLong() : -1L;
            assertEquals(0L, remainingCount, "queue must be empty after pairing — both tickets removed by ZREM");

            // Only ONE pending match exists (the winner's)
            Object pending1 = redis.send(Request.cmd(Command.EXISTS).arg("match-pending:match-race-1"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            Object pending2 = redis.send(Request.cmd(Command.EXISTS).arg("match-pending:match-race-2"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            long count = (pending1 != null && pending1.toString().equals("1") ? 1 : 0)
                    + (pending2 != null && pending2.toString().equals("1") ? 1 : 0);
            assertEquals(1, count, "exactly one pending match must exist (1 ticket → 1 match invariant)");
        } finally {
            // Cleanup
            redis.send(Request.cmd(Command.DEL).arg(queueKey).arg("match-pending:match-race-1").arg("match-pending:match-race-2"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
}
