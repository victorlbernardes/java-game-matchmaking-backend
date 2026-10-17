package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.redis.RedisKeys;
import com.victor.matchmaking.redis.TicketCodec;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;

/**
 * Stress test: 10 concurrent requests with the same Idempotency-Key.
 * The atomic Lua script (create.lua) guarantees exactly 1 ticket is created —
 * all other requests see the index already set and return the same ticketId.
 *
 * Without atomicity, multiple requests would create separate tickets, violating
 * 1 Idempotency-key -> 1 ticket.
 */
class IdempotencyStressTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String IDEM_KEY = "stress-idem-key";
    private static final int REQUEST_COUNT = 10;

    @Test
    void tenRequestsSameIdempotencyKeyCreateOneTicket() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        String idemRedisKey = RedisKeys.idempotencyKey(IDEM_KEY);
        try {
            // Cleanup
            redis.send(Request.cmd(Command.DEL).arg(idemRedisKey).arg("ticket:stress-idem"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            // Load the atomic create script
            String script = new String(getClass().getResourceAsStream("/create.lua").readAllBytes(),
                    StandardCharsets.UTF_8);
            String sha = redis.send(Request.cmd(Command.SCRIPT).arg("LOAD").arg(script))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS).toString();
            assertNotNull(sha, "create.lua must load");

            // 10 concurrent requests with the SAME Idempotency-Key (each has a unique ticketId)
            List<Future<io.vertx.redis.client.Response>> requests = new ArrayList<>();
            for (int i = 0; i < REQUEST_COUNT; i++) {
                MatchmakingTicket ticket = new MatchmakingTicket("ticket-stress-idem-" + i, "p1",
                        com.victor.matchmaking.domain.ticket.GameMode.ONE_V_ONE,
                        com.victor.matchmaking.domain.ticket.Region.NA, 80, 50,
                        java.time.Instant.now(), TicketState.SEARCHING, 0L, null);
                requests.add(redis.send(Request.cmd(Command.EVALSHA).arg(sha)
                        .arg("1").arg(idemRedisKey)
                        .arg(TicketCodec.toJson(ticket).encode())
                        .arg(ticket.id())
                        .arg("queue:1v1:NA")
                        .arg(String.valueOf(ticket.queuedAt().toEpochMilli()))
                        .arg("900").arg("900")));
            }

            // Wait for all 10 requests to complete
            Future.all(requests).toCompletionStage().toCompletableFuture().get(20, TimeUnit.SECONDS);

            // All requests must return the SAME ticketId (the one that won the race)
            String firstResult = requests.get(0).result().toString();
            assertNotNull(firstResult, "first request must return a ticketId");
            for (int i = 1; i < REQUEST_COUNT; i++) {
                String result = requests.get(i).result().toString();
                assertEquals(firstResult, result,
                        "request " + i + " must return the same ticketId as request 0 (idempotent)");
            }

            // Only ONE ticket exists in the store (not 10)
            io.vertx.redis.client.Response ticketCount = redis.send(Request.cmd(Command.EVAL)
                            .arg("local c=0 for _,k in ipairs(redis.call('keys','ticket:ticket-stress-idem-*')) do c=c+1 end return c")
                            .arg("0"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(1L, ticketCount.toLong(), "exactly 1 ticket must exist (1 Idempotency-key -> 1 ticket)");

            // The idempotency index points to the winning ticket
            String idemValue = redis.send(Request.cmd(Command.GET).arg(idemRedisKey))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS).toString();
            assertEquals(firstResult, idemValue, "idempotency index must point to the winning ticket");
        } finally {
            // Cleanup
            for (int i = 0; i < REQUEST_COUNT; i++) {
                redis.send(Request.cmd(Command.DEL).arg("ticket:ticket-stress-idem-" + i))
                        .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            }
            redis.send(Request.cmd(Command.DEL).arg(idemRedisKey))
                    .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
}
