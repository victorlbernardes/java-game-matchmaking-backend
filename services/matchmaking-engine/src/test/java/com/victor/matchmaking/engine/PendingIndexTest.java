package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.engine.application.PendingMatch;
import com.victor.matchmaking.engine.application.PendingMatchStore;
import com.victor.matchmaking.engine.infrastructure.EngineKpis;
import com.victor.matchmaking.engine.infrastructure.RedisPendingMatchStore;
import com.victor.matchmaking.redis.RedisKeys;
import com.victor.matchmaking.redis.RedisPlayerNotifier;
import com.victor.matchmaking.redis.RedisPublisher;
import com.victor.matchmaking.redis.RedisTicketRepository;
import com.victor.matchmaking.redis.TicketRepository;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;

/**
 * The reverse index a reconnecting client depends on (W19, ADR 0026): when the pairing forms a
 * match, every player is pointed at it — {@code player-pending:{playerId}} → {@code matchId} — and
 * the pointer is removed when the match is confirmed or dissolved.
 *
 * <p>The API can only answer "what does this player owe?" by player id; {@code match-pending:{id}}
 * is keyed the other way round and cannot be searched. Without the index the heartbeat has nothing
 * to re-present, which is exactly the state W16 measured: matches dissolving while both services
 * were healthy.
 *
 * <p>The test drives one pairing tick and reads Redis, so it fails if the pairing stops writing the
 * index (the keys are absent) or if the cleanup paths stop removing it (a stale pointer would
 * re-present a match that is already gone).
 */
class PendingIndexTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String QUEUE_KEY = "queue:1v1:NA";
    private static final String TICKET_A = "ticket-index-a";
    private static final String TICKET_B = "ticket-index-b";
    private static final String PLAYER_A = "index-player-a";
    private static final String PLAYER_B = "index-player-b";

    @Test
    void pairingIndexesEveryPlayerAndClearingRemovesThePointers() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            clean(redis);
            MatchmakingEngineApplication.PAIR_SCRIPT = MatchmakingEngineApplication.pairScript(redis);
            assertNotNull(MatchmakingEngineApplication.PAIR_SCRIPT.load()
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS), "pair.lua must load");

            TicketRepository tickets = new RedisTicketRepository(redis);
            PendingMatchStore pending = new RedisPendingMatchStore(redis);
            RedisPlayerNotifier notifier = new RedisPlayerNotifier(new RedisPublisher(redis));
            EngineKpis kpis = new EngineKpis(new SimpleMeterRegistry());

            Instant base = Instant.now();
            MatchmakingTicket ticketA = searching(TICKET_A, PLAYER_A, base);
            MatchmakingTicket ticketB = searching(TICKET_B, PLAYER_B, base);
            save(tickets, ticketA);
            save(tickets, ticketB);
            redis.send(Request.cmd(Command.ZADD).arg(QUEUE_KEY)
                            .arg(String.valueOf(base.toEpochMilli())).arg(TICKET_A)
                            .arg(String.valueOf(base.toEpochMilli())).arg(TICKET_B))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            // The pairing entry point is called with the roster named (not driven through a
            // queue tick): another test class may leave a member behind, and this test is about
            // the index, not about the pairing policy.
            Boolean paired = MatchmakingEngineApplication
                    .pair(List.of(ticketA, ticketB), QUEUE_KEY, redis, tickets, pending, notifier, kpis)
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertTrue(Boolean.TRUE.equals(paired), "the two compatible tickets must pair");

            String matchId = get(redis, RedisKeys.playerPendingKey(PLAYER_A));
            assertTrue(!matchId.isEmpty(), "the pairing must point player A at the match");
            assertEquals(matchId, get(redis, RedisKeys.playerPendingKey(PLAYER_B)),
                    "the pairing must point player B at the match");
            assertTrue(pttl(redis, RedisKeys.playerPendingKey(PLAYER_A)) > 0,
                    "the pointer must expire with the pending match");

            PendingMatch pm = pending.findById(matchId).toCompletionStage().toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
            assertNotNull(pm);
            assertEquals(List.of(PLAYER_A, PLAYER_B),
                    pm.players().stream().map(PendingMatch.PendingPlayer::playerId).toList(),
                    "the indexed match must be the one the two players were paired into");
            pending.clearPlayerIndex(pm).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            pending.delete(matchId).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            assertEquals("", get(redis, RedisKeys.playerPendingKey(PLAYER_A)),
                    "a finished match must leave no pointer behind");
            assertEquals("", get(redis, RedisKeys.playerPendingKey(PLAYER_B)));
        } finally {
            clean(redis);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static void save(TicketRepository tickets, MatchmakingTicket ticket) throws Exception {
        tickets.save(null, ticket).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static MatchmakingTicket searching(String ticketId, String playerId, Instant queuedAt) {
        return new MatchmakingTicket(ticketId, playerId, GameMode.ONE_V_ONE, Region.NA, 80, 50,
                queuedAt, TicketState.SEARCHING, 0L, null);
    }

    private static String get(Redis redis, String key) throws Exception {
        Response response = redis.send(Request.cmd(Command.GET).arg(key))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return response != null ? response.toString() : "";
    }

    private static long pttl(Redis redis, String key) throws Exception {
        Response response = redis.send(Request.cmd(Command.PTTL).arg(key))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return response != null ? response.toLong() : -1;
    }

    private static void clean(Redis redis) throws Exception {
        redis.send(Request.cmd(Command.DEL).arg(QUEUE_KEY)
                        .arg("ticket:" + TICKET_A).arg("ticket:" + TICKET_B)
                        .arg(RedisKeys.playerPendingKey(PLAYER_A))
                        .arg(RedisKeys.playerPendingKey(PLAYER_B)))
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
