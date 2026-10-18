package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.engine.application.PendingMatchStore;
import com.victor.matchmaking.engine.infrastructure.EngineKpis;
import com.victor.matchmaking.engine.infrastructure.RedisPendingMatchStore;
import com.victor.matchmaking.redis.RedisPlayerNotifier;
import com.victor.matchmaking.redis.RedisPublisher;
import com.victor.matchmaking.redis.RedisTicketRepository;
import com.victor.matchmaking.redis.TicketRepository;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;

/**
 * W15 found the stall, W16 fixes it: a ticket expires on its 900 s TTL, but its queue member
 * cannot, so the member stays behind as a ghost. Read as the head of the queue it loads as a
 * missing ticket and blocks every compatible player behind it — the whole mode/region stops
 * pairing.
 *
 * <p>The test puts a ghost at the head of {@code queue:1v1:NA}, two compatible live tickets
 * behind it, and runs one pairing tick: the queue must pair, the ghost must be gone and both
 * players must end up in the same pending match. Without the reaper there is no pending match
 * at all — the live players wait forever.
 */
class StaleQueueMemberTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String QUEUE_KEY = "queue:1v1:NA";
    private static final String GHOST_TICKET = "ticket-stale-ghost";
    private static final String LIVE_TICKET_A = "ticket-stale-a";
    private static final String LIVE_TICKET_B = "ticket-stale-b";

    @Test
    void aGhostAtTheHeadOfTheQueueDoesNotBlockThePairing() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        List<String> createdPending = new ArrayList<>();
        try {
            clean(redis);
            // The app loads the script at startup; the test runs the same pairing tick by hand.
            MatchmakingEngineApplication.PAIR_SCRIPT_SHA = MatchmakingEngineApplication.loadPairScript(redis)
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertNotNull(MatchmakingEngineApplication.PAIR_SCRIPT_SHA, "pair.lua must load");

            TicketRepository tickets = new RedisTicketRepository(redis);
            PendingMatchStore pending = new RedisPendingMatchStore(redis);
            RedisPlayerNotifier notifier = new RedisPlayerNotifier(new RedisPublisher(redis));
            EngineKpis kpis = new EngineKpis(new SimpleMeterRegistry());

            Instant base = Instant.now();
            tickets.save(null, searching(LIVE_TICKET_A, "stale-player-a", base))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            tickets.save(null, searching(LIVE_TICKET_B, "stale-player-b", base))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            // Exactly what the TTL leaves behind: the member, with the lowest score (the head of
            // the queue), and no ticket behind it.
            redis.send(Request.cmd(Command.ZADD).arg(QUEUE_KEY)
                            .arg(String.valueOf(base.minusSeconds(10).toEpochMilli())).arg(GHOST_TICKET))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(3L, zcard(redis), "the queue must hold the ghost and the two live tickets");

            // One tick of the pairing loop, the way the 500 ms timer runs it.
            MatchmakingEngineApplication.tryPairFromQueue(QUEUE_KEY, redis, tickets, pending, notifier, kpis);

            String matchId = awaitPendingMatch(redis, 10);
            assertNotNull(matchId, "the two live tickets must pair even with a ghost at the head");
            createdPending.add(matchId);

            JsonObject match = new JsonObject(get(redis, "match-pending:" + matchId));
            List<String> players = new ArrayList<>();
            for (Object entry : match.getJsonArray("players")) {
                players.add(((JsonObject) entry).getString("playerId"));
            }
            players.sort(String::compareTo);
            assertEquals(List.of("stale-player-a", "stale-player-b"), players,
                    "both live players must be in the match");

            assertEquals(0L, zcard(redis), "the ghost and the paired members must be off the queue");
            assertTrue(get(redis, "ticket:" + GHOST_TICKET).isEmpty(),
                    "the ghost never had a ticket - the point is that it no longer had a member");
        } finally {
            clean(redis);
            for (String matchId : createdPending) {
                redis.send(Request.cmd(Command.DEL).arg("match-pending:" + matchId))
                        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static MatchmakingTicket searching(String ticketId, String playerId, Instant queuedAt) {
        return new MatchmakingTicket(ticketId, playerId, GameMode.ONE_V_ONE, Region.NA, 80, 50,
                queuedAt, TicketState.SEARCHING, 0L, null);
    }

    private static long zcard(Redis redis) throws Exception {
        Response response = redis.send(Request.cmd(Command.ZCARD).arg(QUEUE_KEY))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return response != null ? response.toLong() : 0L;
    }

    private static String get(Redis redis, String key) throws Exception {
        Response response = redis.send(Request.cmd(Command.GET).arg(key))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return response != null ? response.toString() : "";
    }

    /** The pairing is async (event loop); the test waits for the effect, not for a callback. */
    private static String awaitPendingMatch(Redis redis, int seconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
        while (System.nanoTime() < deadline) {
            Response keys = redis.send(Request.cmd(Command.KEYS).arg("match-pending:*"))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            if (keys != null && keys.size() > 0) {
                return keys.get(0).toString().substring("match-pending:".length());
            }
            TimeUnit.MILLISECONDS.sleep(100);
        }
        return null;
    }

    private static void clean(Redis redis) throws Exception {
        redis.send(Request.cmd(Command.DEL).arg(QUEUE_KEY)
                        .arg("ticket:" + GHOST_TICKET)
                        .arg("ticket:" + LIVE_TICKET_A)
                        .arg("ticket:" + LIVE_TICKET_B))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        Response pending = redis.send(Request.cmd(Command.KEYS).arg("match-pending:*"))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        if (pending != null) {
            for (int i = 0; i < pending.size(); i++) {
                redis.send(Request.cmd(Command.DEL).arg(pending.get(i).toString()))
                        .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
        }
    }
}
