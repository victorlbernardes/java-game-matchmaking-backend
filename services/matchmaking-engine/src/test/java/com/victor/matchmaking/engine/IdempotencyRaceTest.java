package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.redis.RedisKeys;
import com.victor.matchmaking.redis.TicketCodec;

import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;

/**
 * Race-condition test: two requests with the same Idempotency-Key.
 * The atomic Lua script (create.lua) guarantees that only ONE ticket is created —
 * the second request sees the idempotency index already set and returns the
 * existing ticketId.
 *
 * If the script is removed (falls back to non-atomic GET + save), BOTH requests
 * would create separate tickets, violating 1 Idempotency-Key → 1 ticket.
 */
class IdempotencyRaceTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String IDEM_KEY = "race-idem-key";

    @Test
    void sameIdempotencyKeyCreatesOnlyOneTicket() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        String idemRedisKey = RedisKeys.idempotencyKey(IDEM_KEY);
        try {
            // Cleanup
            redis.send(Request.cmd(Command.DEL).arg(idemRedisKey).arg("ticket-race-idem-a").arg("ticket-race-idem-b"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            // Load the atomic create script
            String script = new String(getClass().getResourceAsStream("/create.lua").readAllBytes(),
                    StandardCharsets.UTF_8);
            String sha = redis.send(Request.cmd(Command.SCRIPT).arg("LOAD").arg(script))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS).toString();
            assertNotNull(sha, "create.lua must load");

            // Two requests with the SAME Idempotency-Key (simulated sequentially — Redis is single-threaded)
            MatchmakingTicket ticketA = new MatchmakingTicket("ticket-race-idem-a", "p1",
                    com.victor.matchmaking.domain.ticket.GameMode.ONE_V_ONE,
                    com.victor.matchmaking.domain.ticket.Region.NA, 80, 50,
                    java.time.Instant.now(), TicketState.SEARCHING, 0L, null);
            MatchmakingTicket ticketB = new MatchmakingTicket("ticket-race-idem-b", "p1",
                    com.victor.matchmaking.domain.ticket.GameMode.ONE_V_ONE,
                    com.victor.matchmaking.domain.ticket.Region.NA, 80, 50,
                    java.time.Instant.now(), TicketState.SEARCHING, 0L, null);

            // Request 1: creates the ticket
            Object result1 = redis.send(Request.cmd(Command.EVALSHA).arg(sha)
                            .arg("1").arg(idemRedisKey)
                            .arg(TicketCodec.toJson(ticketA).encode())
                            .arg(ticketA.id())
                            .arg("queue:1v1:NA")
                            .arg(String.valueOf(ticketA.queuedAt().toEpochMilli()))
                            .arg("900").arg("900"))
                    .toCompletionStage().toCompletableFuture().get(15, TimeUnit.SECONDS);
            assertNotNull(result1, "request 1 must return a ticketId");
            assertEquals("ticket-race-idem-a", result1.toString(), "request 1 must create ticketA");

            // Request 2: same Idempotency-Key — the atomic script returns the EXISTING ticketId
            Object result2 = redis.send(Request.cmd(Command.EVALSHA).arg(sha)
                            .arg("1").arg(idemRedisKey)
                            .arg(TicketCodec.toJson(ticketB).encode())
                            .arg(ticketB.id())
                            .arg("queue:1v1:NA")
                            .arg(String.valueOf(ticketB.queuedAt().toEpochMilli()))
                            .arg("900").arg("900"))
                    .toCompletionStage().toCompletableFuture().get(15, TimeUnit.SECONDS);
            assertNotNull(result2, "request 2 must return a ticketId");
            assertEquals("ticket-race-idem-a", result2.toString(),
                    "request 2 must return the SAME ticketId (idempotent)");

            // Only ONE ticket exists in the store (not two)
            io.vertx.redis.client.Response ticketCount = redis.send(Request.cmd(Command.EXISTS)
                            .arg("ticket:ticket-race-idem-a").arg("ticket:ticket-race-idem-b"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            long count = ticketCount != null ? ticketCount.toLong() : 0L;
            assertEquals(1L, count, "exactly one ticket must exist (1 Idempotency-Key → 1 ticket invariant)");
        } finally {
            redis.send(Request.cmd(Command.DEL).arg(idemRedisKey).arg("ticket:ticket-race-idem-a")
                            .arg("ticket:ticket-race-idem-b"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
}
