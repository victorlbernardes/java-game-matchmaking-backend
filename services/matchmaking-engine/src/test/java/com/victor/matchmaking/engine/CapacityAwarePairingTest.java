package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.junit.jupiter.api.Test;

import com.victor.matchmaking.domain.ticket.GameMode;
import com.victor.matchmaking.domain.ticket.MatchmakingTicket;
import com.victor.matchmaking.domain.ticket.Region;
import com.victor.matchmaking.domain.ticket.ServedRegions;
import com.victor.matchmaking.domain.ticket.TicketState;
import com.victor.matchmaking.engine.application.Capacity;
import com.victor.matchmaking.engine.application.PendingMatchStore;
import com.victor.matchmaking.engine.infrastructure.EngineKpis;
import com.victor.matchmaking.engine.infrastructure.RedisPendingMatchStore;
import com.victor.matchmaking.engine.infrastructure.WriteGate;
import com.victor.matchmaking.metrics.MetricNames;
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
 * Property: with a pool that has no room, the engine forms nothing and the queue waits (W22,
 * ADR 0031).
 *
 * <p>The failure this pins is the one that costs a player twice: forming a match the region cannot
 * host, because the player then confirms, the confirmation deadline passes with no server, and the
 * match dissolves — the match is lost <em>and</em> so is the wait. "Waiting" is the better outcome,
 * and it is still something an operator must see, which is what the suspension counter is for.
 *
 * <p>The second half is what makes the first meaningful: the same queue pairs the moment a slot
 * exists, and a batch never forms more matches than the pool has room for. Capacity is a one-line
 * lambda here on purpose — the port is the whole contract, and the Postgres read behind it is
 * asserted in {@code GameServerRepositoryTest}.
 */
class CapacityAwarePairingTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final GameMode MODE = GameMode.ONE_V_ONE;

    @Test
    void noSlotFormsNothingAndTheQueueWaits() throws Exception {
        withEngine((vertx, redis, tickets, pending, notifier, kpis, registry) -> {
            Instant base = Instant.now();
            for (int i = 0; i < 2; i++) {
                await(tickets.save("w22-wait-" + i, ticket("w22-wait-" + i, "w22-wait-player-" + i, base)));
            }
            AtomicInteger slots = new AtomicInteger(0);

            pair(vertx, redis, tickets, pending, notifier, kpis, capacity(slots));

            assertEquals(2, depth(redis), "both players are still in the queue");
            assertEquals(0, count(redis, "match-pending:*"), "no match was formed");
            assertTrue(suspensions(registry) > 0, "and the reason is counted, not silent");
        });
    }

    @Test
    void oneSlotFormsExactlyOneMatchEvenWithPlayersEnoughForTwo() throws Exception {
        withEngine((vertx, redis, tickets, pending, notifier, kpis, registry) -> {
            Instant base = Instant.now();
            for (int i = 0; i < 4; i++) {
                await(tickets.save("w22-once-" + i, ticket("w22-once-" + i, "w22-once-player-" + i, base)));
            }
            AtomicInteger slots = new AtomicInteger(1);

            pair(vertx, redis, tickets, pending, notifier, kpis, capacity(slots));

            waitUntil(() -> countQuietly(redis, "match-pending:*") == 1, "the one slot must be used");
            assertEquals(1, count(redis, "match-pending:*"),
                    "one slot is one match: forming two would leave one waiting for a server");
            assertEquals(2, depth(redis), "the other two players keep waiting");
        });
    }

    @Test
    void anUnknownPoolConstrainsNothing() throws Exception {
        withEngine((vertx, redis, tickets, pending, notifier, kpis, registry) -> {
            Instant base = Instant.now();
            await(tickets.save("w22-open-a", ticket("w22-open-a", "w22-open-player-a", base)));
            await(tickets.save("w22-open-b", ticket("w22-open-b", "w22-open-player-b", base)));

            // Capacity.UNLIMITED is what a deployment with no game server pool behaves like: the
            // engine of W1-W21, unchanged.
            pair(vertx, redis, tickets, pending, notifier, kpis, Capacity.UNLIMITED);

            waitUntil(() -> countQuietly(redis, "match-pending:*") == 1, "an unknown pool must still pair");
            assertEquals(0, depth(redis));
        });
    }

    @Test
    void theQueuePairsWhenTheSlotComesBack() throws Exception {
        withEngine((vertx, redis, tickets, pending, notifier, kpis, registry) -> {
            Instant base = Instant.now();
            await(tickets.save("w22-back-a", ticket("w22-back-a", "w22-back-player-a", base)));
            await(tickets.save("w22-back-b", ticket("w22-back-b", "w22-back-player-b", base)));
            AtomicInteger slots = new AtomicInteger(0);

            pair(vertx, redis, tickets, pending, notifier, kpis, capacity(slots));
            assertEquals(0, count(redis, "match-pending:*"), "nothing to run a match on");

            // A match ended somewhere and the slot went back to the pool (the orchestrator's
            // release): the very next tick pairs the players who were waiting for it.
            slots.set(1);
            pair(vertx, redis, tickets, pending, notifier, kpis, capacity(slots));

            waitUntil(() -> countQuietly(redis, "match-pending:*") == 1,
                    "a freed slot must be used by the players who waited for it");
            assertEquals(0, depth(redis), "and they leave the queue");
        });
    }

    private static Capacity capacity(AtomicInteger slots) {
        return (gameMode, region) -> region == Region.NA ? OptionalInt.of(slots.get()) : OptionalInt.empty();
    }

    private static double suspensions(SimpleMeterRegistry registry) {
        var counter = registry.find(MetricNames.PAIRING_SUSPENDED)
                .tag(MetricNames.TAG_MODE, MODE.label())
                .tag(MetricNames.TAG_REGION, Region.NA.name())
                .counter();
        return counter == null ? 0.0 : counter.count();
    }

    private static void pair(Vertx vertx, Redis redis, TicketRepository tickets, PendingMatchStore pending,
            RedisPlayerNotifier notifier, EngineKpis kpis, Capacity capacity) {
        MatchmakingEngineApplication.pairAcrossQueues(ServedRegions.of(Region.NA), vertx, redis, tickets,
                pending, notifier, kpis, WriteGate.unbounded(), capacity);
    }

    @FunctionalInterface
    interface EngineBody {
        void run(Vertx vertx, Redis redis, TicketRepository tickets, PendingMatchStore pending,
                RedisPlayerNotifier notifier, EngineKpis kpis, SimpleMeterRegistry registry) throws Exception;
    }

    private static void withEngine(EngineBody body) throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            clean(redis);
            MatchmakingEngineApplication.PAIR_SCRIPT = MatchmakingEngineApplication.pairScript(redis);
            assertNotNull(await(MatchmakingEngineApplication.PAIR_SCRIPT.load()), "pair.lua must load");

            TicketRepository tickets = new RedisTicketRepository(redis, RedisTicketRepository.UNBOUNDED_QUEUE, 1);
            PendingMatchStore pending = new RedisPendingMatchStore(redis);
            RedisPlayerNotifier notifier = new RedisPlayerNotifier(new RedisPublisher(redis));
            SimpleMeterRegistry registry = new SimpleMeterRegistry();
            EngineKpis kpis = new EngineKpis(registry);

            body.run(vertx, redis, tickets, pending, notifier, kpis, registry);
        } finally {
            clean(redis);
            redis.close();
            vertx.close();
        }
    }

    private static MatchmakingTicket ticket(String id, String playerId, Instant queuedAt) {
        return new MatchmakingTicket(id, playerId, MODE, Region.NA, 80, 50, queuedAt, TicketState.SEARCHING, 0L,
                null);
    }

    private static long depth(Redis redis) throws Exception {
        Response size = redis.send(Request.cmd(Command.ZCARD).arg(RedisKeys.queueKey(MODE.label(), Region.NA.name())))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return size != null ? size.toLong() : 0L;
    }

    private static int countQuietly(Redis redis, String pattern) {
        try {
            return count(redis, pattern);
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

    private static int count(Redis redis, String pattern) throws Exception {
        Response found = redis.send(Request.cmd(Command.KEYS).arg(pattern))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return found != null ? found.size() : 0;
    }

    private static void clean(Redis redis) throws Exception {
        List<String> toDelete = new ArrayList<>();
        for (GameMode mode : GameMode.values()) {
            for (Region region : Region.values()) {
                toDelete.add(RedisKeys.queueKey(mode.label(), region.name()));
            }
        }
        for (String pattern : new String[] { "ticket:w22-*", "match-pending:*" }) {
            Response found = redis.send(Request.cmd(Command.KEYS).arg(pattern))
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            for (int i = 0; found != null && i < found.size(); i++) {
                toDelete.add(found.get(i).toString());
            }
        }
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
