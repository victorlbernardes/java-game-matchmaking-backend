package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.ServedRegions;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.engine.application.PendingMatchStore;
import com.victor.matchmaking.engine.infrastructure.EngineKpis;
import com.victor.matchmaking.engine.infrastructure.RedisPendingMatchStore;
import com.victor.matchmaking.engine.infrastructure.WriteGate;
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
 * Property: an engine pairs only the regions it is deployed for (W21, ADR 0039).
 *
 * <p>Two compatible players in a region this engine does not serve must stay in the queue, and the
 * same two must pair the moment the engine does serve it. The second half is what makes the first
 * meaningful: it is not "EU never pairs", it is "EU is not this engine's".
 *
 * <p>The failure this pins is the one a region boundary exists to prevent: an engine that pairs a
 * queue it does not own forms a match on a region it cannot host, and the player is sent to a game
 * server that is not there. It also keeps the KPIs honest - a zero published for a queue the engine
 * never reads is a dashboard that lies politely.
 */
class EngineRegionScopeTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final GameMode MODE = GameMode.ONE_V_ONE;

    @Test
    void anEnginePairsOnlyTheRegionsItServes() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            clean(redis);
            MatchmakingEngineApplication.PAIR_SCRIPT = MatchmakingEngineApplication.pairScript(redis);
            assertNotNull(await(MatchmakingEngineApplication.PAIR_SCRIPT.load()), "pair.lua must load");

            TicketRepository tickets = new RedisTicketRepository(redis,
                    RedisTicketRepository.UNBOUNDED_QUEUE, 1);
            PendingMatchStore pending = new RedisPendingMatchStore(redis);
            RedisPlayerNotifier notifier = new RedisPlayerNotifier(new RedisPublisher(redis));
            EngineKpis kpis = new EngineKpis(new SimpleMeterRegistry());

            Instant base = Instant.now();
            await(tickets.save("w21-na-a", ticket("w21-na-a", "w21-na-a-player", Region.NA, base)));
            await(tickets.save("w21-na-b", ticket("w21-na-b", "w21-na-b-player", Region.NA, base)));
            await(tickets.save("w21-eu-a", ticket("w21-eu-a", "w21-eu-a-player", Region.EU, base)));
            await(tickets.save("w21-eu-b", ticket("w21-eu-b", "w21-eu-b-player", Region.EU, base)));
            assertEquals(2, depth(redis, Region.NA));
            assertEquals(2, depth(redis, Region.EU));

            MatchmakingEngineApplication.pairAcrossQueues(ServedRegions.of(Region.NA), vertx, redis, tickets,
                    pending, notifier, kpis, WriteGate.unbounded());

            waitUntil(() -> depthQuietly(redis, Region.NA) == 0, "the served region must pair");
            assertEquals(0, depth(redis, Region.NA), "the served region is drained");
            assertEquals(2, depth(redis, Region.EU),
                    "an engine deployed for NA must not pair a region it does not serve");
            assertEquals(1, count(redis, "match-pending:*"), "only the served region formed a match");

            // And the other direction, with the same code: serving EU pairs the EU pair.
            MatchmakingEngineApplication.pairAcrossQueues(ServedRegions.of(Region.EU), vertx, redis, tickets,
                    pending, notifier, kpis, WriteGate.unbounded());

            waitUntil(() -> depthQuietly(redis, Region.EU) == 0, "the other region must pair when served");
            assertEquals(0, depth(redis, Region.EU));
            assertEquals(2, count(redis, "match-pending:*"));
        } finally {
            clean(redis);
            redis.close();
            vertx.close();
        }
    }

    private static MatchmakingTicket ticket(String id, String playerId, Region region, Instant queuedAt) {
        return new MatchmakingTicket(id, playerId, MODE, region, 80, 50, queuedAt, TicketState.SEARCHING, 0L,
                null);
    }

    private static long depth(Redis redis, Region region) throws Exception {
        return zcard(redis, RedisKeys.queueKey(MODE.label(), region.name()));
    }

    /** The polling wait reads the queue from a non-test thread, so it may not throw. */
    private static long depthQuietly(Redis redis, Region region) {
        try {
            return depth(redis, region);
        } catch (Exception failure) {
            return -1;
        }
    }

    private static void waitUntil(BooleanSupplier condition, String what) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("timed out waiting: " + what);
    }

    private static long zcard(Redis redis, String key) throws Exception {
        Response size = redis.send(Request.cmd(Command.ZCARD).arg(key))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return size != null ? size.toLong() : 0L;
    }

    private static int count(Redis redis, String pattern) throws Exception {
        Response found = redis.send(Request.cmd(Command.KEYS).arg(pattern))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return found != null ? found.size() : 0;
    }

    private static void clean(Redis redis) throws Exception {
        List<String> toDelete = new ArrayList<>();
        for (Region region : Region.values()) {
            toDelete.add(RedisKeys.queueKey(MODE.label(), region.name()));
        }
        for (String key : new String[] { "ticket:w21-*", "match-pending:*" }) {
            Response found = redis.send(Request.cmd(Command.KEYS).arg(key))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            for (int i = 0; found != null && i < found.size(); i++) {
                toDelete.add(found.get(i).toString());
            }
        }
        assertTrue(!toDelete.isEmpty(), "there is always at least one queue key to clear");
        Request del = Request.cmd(Command.DEL);
        for (String key : toDelete) {
            del.arg(key);
        }
        redis.send(del).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    private static <T> T await(io.vertx.core.Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }
}
