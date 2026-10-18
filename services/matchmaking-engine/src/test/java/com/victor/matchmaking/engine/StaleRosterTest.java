package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
 * A pairing takes exactly the players it paired.
 *
 * <p>The script used to take "the head of the queue" itself while the caller stored the roster it
 * had read a moment earlier. Two ticks whose reads overlapped then removed the head twice: the
 * second stored the first one's roster, so two players nobody had paired were taken off the queue
 * (out of every match, waiting for nothing) and the first pair was in two matches at once. Under
 * the W16 drain loop - many pairings per tick, ticks overlapping - that was 40 matches refused as
 * "tickets not in CONFIRMING", 40 dissolved and 186 tickets off the queue with nobody to pair
 * them.
 *
 * <p>The test stages exactly that overlap: a roster is read, a tick pairs it, and a second tick
 * that read the same roster offers it again. The second call must take nobody, and the two
 * players behind it must still be waiting. Without the members being checked, the stale call
 * takes them and stores a pending match for players who are already paired.
 */
class StaleRosterTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final String QUEUE_KEY = "queue:1v1:NA";
    private static final List<String> TICKETS = List.of(
            "ticket-stale-roster-a", "ticket-stale-roster-b",
            "ticket-stale-roster-c", "ticket-stale-roster-d");

    @Test
    void aPairingWhosePlayersAreGoneTakesNobodyElse() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            clean(redis);
            MatchmakingEngineApplication.PAIR_SCRIPT = MatchmakingEngineApplication.pairScript(redis);
            String pairScriptSha = MatchmakingEngineApplication.PAIR_SCRIPT.load()
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertNotNull(pairScriptSha, "pair.lua must load");

            TicketRepository tickets = new RedisTicketRepository(redis);
            PendingMatchStore pending = new RedisPendingMatchStore(redis);
            RedisPlayerNotifier notifier = new RedisPlayerNotifier(new RedisPublisher(redis));
            EngineKpis kpis = new EngineKpis(new SimpleMeterRegistry());

            Instant base = Instant.now();
            for (int i = 0; i < TICKETS.size(); i++) {
                await(tickets.save(null, searching(TICKETS.get(i), "stale-roster-player-" + i,
                        base.plusMillis(i))));
            }
            assertEquals(4L, zcard(redis), "all four tickets are waiting");

            // Two ticks read the same head before either of them paired it.
            List<MatchmakingTicket> staleRoster = List.of(
                    await(tickets.findById(TICKETS.get(0))), await(tickets.findById(TICKETS.get(1))));

            assertTrue(await(MatchmakingEngineApplication.pair(staleRoster, QUEUE_KEY, redis, tickets,
                            pending, notifier, kpis)),
                    "the first tick pairs the roster it read");
            assertFalse(await(MatchmakingEngineApplication.pair(staleRoster, QUEUE_KEY, redis, tickets,
                            pending, notifier, kpis)),
                    "the second tick must pair nobody: the players it had read are already paired");

            assertEquals(2L, zcard(redis),
                    "c and d are still waiting: a pairing with stale players must take none");
            MatchmakingTicket third = await(tickets.findById(TICKETS.get(2)));
            assertNotNull(third, "c is still a ticket");
            assertEquals(TicketState.SEARCHING, third.status(),
                    "a player taken off the queue must have been paired, and c was not");
            assertEquals(1, pendingMatches(redis),
                    "one pending match: the second caller stored the same roster again");
            assertEquals(List.of("stale-roster-player-0", "stale-roster-player-1"),
                    playersOfTheOnlyPendingMatch(redis), "the pending match is the roster that won");
        } finally {
            clean(redis);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static MatchmakingTicket searching(String ticketId, String playerId, Instant queuedAt) {
        return new MatchmakingTicket(ticketId, playerId, GameMode.ONE_V_ONE, Region.NA, 80, 50,
                queuedAt, TicketState.SEARCHING, 0L, null);
    }

    private static long zcard(Redis redis) throws Exception {
        Response response = await(redis.send(Request.cmd(Command.ZCARD).arg(QUEUE_KEY)));
        return response != null ? response.toLong() : 0L;
    }

    private static int pendingMatches(Redis redis) throws Exception {
        Response keys = await(redis.send(Request.cmd(Command.KEYS).arg("match-pending:*")));
        return keys == null ? 0 : keys.size();
    }

    private static List<String> playersOfTheOnlyPendingMatch(Redis redis) throws Exception {
        Response keys = await(redis.send(Request.cmd(Command.KEYS).arg("match-pending:*")));
        JsonObject match = new JsonObject(await(redis.send(Request.cmd(Command.GET)
                .arg(keys.get(0).toString()))).toString());
        return match.getJsonArray("players").stream()
                .map(entry -> ((JsonObject) entry).getString("playerId"))
                .toList();
    }

    private static void clean(Redis redis) throws Exception {
        await(redis.send(Request.cmd(Command.DEL).arg(QUEUE_KEY)));
        for (String ticket : TICKETS) {
            await(redis.send(Request.cmd(Command.DEL).arg("ticket:" + ticket)));
        }
        Response keys = await(redis.send(Request.cmd(Command.KEYS).arg("match-pending:*")));
        if (keys != null) {
            for (int i = 0; i < keys.size(); i++) {
                await(redis.send(Request.cmd(Command.DEL).arg(keys.get(i).toString())));
            }
        }
    }

    private static <T> T await(io.vertx.core.Future<T> future) {
        return future.toCompletionStage().toCompletableFuture().join();
    }
}
