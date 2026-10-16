package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;

/**
 * Stress test: 10 workers pairing simultaneously from a queue of 20 tickets.
 * The atomic Lua script guarantees each ticket ends up in at most 1 match.
 *
 * Without atomicity, multiple workers could pair the same tickets, creating
 * multiple pending matches and violating 1 ticket -> 1 match.
 */
class PairingStressTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final int WORKER_COUNT = 10;
    private static final int TICKET_COUNT = 20;

    @Test
    void tenWorkersPairingStressTest() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        String queueKey = "queue:1v1:NA";
        try {
            // Load the atomic pairing script
            String script = new String(getClass().getResourceAsStream("/pair.lua").readAllBytes(),
                    StandardCharsets.UTF_8);
            String sha = redis.send(Request.cmd(Command.SCRIPT).arg("LOAD").arg(script))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS).toString();
            assertNotNull(sha, "pair.lua must load");

            // Setup: 20 tickets in the queue
            redis.send(Request.cmd(Command.DEL).arg(queueKey)).toCompletionStage().toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
            for (int i = 0; i < TICKET_COUNT; i++) {
                String ticketId = String.format("ticket-stress-%03d", i);
                redis.send(Request.cmd(Command.SET).arg("ticket:" + ticketId)
                                .arg("{\"matchId\":\"" + ticketId + "\"}").arg("EX").arg("900"))
                        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
                redis.send(Request.cmd(Command.ZADD).arg(queueKey).arg(String.valueOf(i * 1000)).arg(ticketId))
                        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }

            // 10 workers race to pair — each calls the script once with a unique matchId
            List<Future<io.vertx.redis.client.Response>> workers = new ArrayList<>();
            for (int i = 0; i < WORKER_COUNT; i++) {
                String matchId = "match-stress-" + i;
                workers.add(redis.send(Request.cmd(Command.EVALSHA).arg(sha)
                        .arg("1").arg(queueKey)
                        .arg(matchId)
                        .arg("{\"players\":[],\"expiresAt\":\"2030-01-01T00:00:00Z\"}")
                        .arg("30")));
            }

            // Wait for all 10 workers to complete
            Future.all(workers).toCompletionStage().toCompletableFuture().get(20, TimeUnit.SECONDS);

            // Count wins (returned a matchId) and losses (returned null)
            int wins = 0;
            int losses = 0;
            for (Future<io.vertx.redis.client.Response> worker : workers) {
                io.vertx.redis.client.Response result = worker.result();
                if (result != null && result.toString().startsWith("match-stress-")) {
                    wins++;
                } else {
                    losses++;
                }
            }

            // With 20 tickets and 10 workers, all 10 workers win (20/2 = 10 pairs formed)
            int expectedWins = TICKET_COUNT / 2;
            assertEquals(expectedWins, wins, "exactly 10 workers should win (20 tickets / 2 per match)");
            assertEquals(WORKER_COUNT - expectedWins, losses, "exactly 0 workers should lose");

            // The queue must be empty — all 20 tickets were removed by the winners' ZREM
            io.vertx.redis.client.Response queueSize = redis.send(Request.cmd(Command.ZCARD).arg(queueKey))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(0L, queueSize.toLong(), "queue must be empty after all pairs are matched");

            // Exactly TICKET_COUNT / 2 = 10 pending matches exist (one per winning worker)
            io.vertx.redis.client.Response pendingCount = redis.send(Request.cmd(Command.EVAL)
                            .arg("local c=0 for _,k in ipairs(redis.call('keys','match-pending:*')) do c=c+1 end return c")
                            .arg("0"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(TICKET_COUNT / 2, pendingCount.toLong(), "exactly 10 pending matches must exist");
        } finally {
            // Cleanup
            for (int i = 0; i < TICKET_COUNT; i++) {
                redis.send(Request.cmd(Command.DEL).arg("ticket:ticket-stress-" + String.format("%03d", i)))
                        .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            }
            for (int i = 0; i < WORKER_COUNT; i++) {
                redis.send(Request.cmd(Command.DEL).arg("match-pending:match-stress-" + i))
                        .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            }
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
}
