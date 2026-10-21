package com.victor.matchmaking.engine;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Instant;
import java.util.ArrayList;
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
import com.victor.matchmaking.redis.QueueFullException;
import com.victor.matchmaking.redis.QueuePartitions;
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
 * Property: spreading a queue over partitions changes where a ticket is stored, never whether it
 * can be paired (W20, ADR 0028).
 *
 * <p>The partition key is a storage decision. The moment it becomes a matching boundary, a player
 * waits forever because a compatible partner landed in another key — a queue that is faster and
 * wrong. So the four properties here are, in order:
 *
 * <ol>
 *   <li>Two players in <b>different partitions</b> still pair, and both members leave their own
 *       key in one atomic step. The script is called with every partition as {@code KEYS}.</li>
 *   <li>A <b>party</b> stays whole in one partition: its members hash by {@code partyId}, so the
 *       atomic party enqueue is still one key and a party is never split across keys.</li>
 *   <li>{@code queueDepth} is the <b>sum</b> over partitions — the number the SLOs and the
 *       admission cap already use must not start answering about one shard.</li>
 *   <li>The <b>cap</b> still holds: the sum over partitions never exceeds the configured depth
 *       ({@code P·floor(C/P)}), because a partitioned queue that admits {@code P} times the cap is
 *       a queue with no boundary at all.</li>
 * </ol>
 */
class PartitionedQueueTest {

    private static final String REDIS_URI = System.getenv().getOrDefault("REDIS_URI", "redis://localhost:6379");
    private static final GameMode MODE = GameMode.ONE_V_ONE;
    private static final Region REGION = Region.NA;
    private static final int PARTITIONS = 8;
    private static final int CAPACITY = 24;

    @Test
    void playersInDifferentPartitionsStillPair() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            clean(redis);
            MatchmakingEngineApplication.PAIR_SCRIPT = MatchmakingEngineApplication.pairScript(redis);
            assertNotNull(MatchmakingEngineApplication.PAIR_SCRIPT.load()
                    .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS), "pair.lua must load");

            TicketRepository tickets = new RedisTicketRepository(redis, RedisTicketRepository.UNBOUNDED_QUEUE,
                    PARTITIONS);
            PendingMatchStore pending = new RedisPendingMatchStore(redis);
            RedisPlayerNotifier notifier = new RedisPlayerNotifier(new RedisPublisher(redis));
            EngineKpis kpis = new EngineKpis(new SimpleMeterRegistry());
            List<String> queueKeys = RedisKeys.queueKeys(MODE.label(), REGION.name(), PARTITIONS);

            String playerA = playerInPartition(0);
            String playerB = playerInPartition(1);
            Instant base = Instant.now();
            MatchmakingTicket ticketA = ticket("w20-a", playerA, base, null);
            MatchmakingTicket ticketB = ticket("w20-b", playerB, base, null);
            await(tickets.save("w20-a", ticketA));
            await(tickets.save("w20-b", ticketB));

            String keyA = partitionKeyOf(playerA);
            String keyB = partitionKeyOf(playerB);
            assertTrue(!keyA.equals(keyB), "the two players must land in different partitions");
            assertEquals(1, zcard(redis, keyA));
            assertEquals(1, zcard(redis, keyB));

            Boolean paired = await(MatchmakingEngineApplication.pair(List.of(ticketA, ticketB), queueKeys,
                    redis, tickets, pending, notifier, kpis));
            assertTrue(Boolean.TRUE.equals(paired),
                    "a roster spanning partitions must pair: the partitions are storage, not a boundary");

            assertEquals(0, zcard(redis, keyA), "the paired member must leave its own partition");
            assertEquals(0, zcard(redis, keyB));
            List<String> pendingKeys = keys(redis, "match-pending:*");
            assertEquals(1, pendingKeys.size(), "exactly one pending match must exist");
            PendingMatch pm = await(pending.findById(pendingKeys.get(0).substring("match-pending:".length())));
            assertNotNull(pm);
            assertEquals(List.of(playerA, playerB),
                    pm.players().stream().map(PendingMatch.PendingPlayer::playerId).toList());
        } finally {
            clean(redis);
            redis.close();
            vertx.close();
        }
    }

    @Test
    void aPartyStaysWholeInOnePartition() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            clean(redis);
            TicketRepository tickets = new RedisTicketRepository(redis, RedisTicketRepository.UNBOUNDED_QUEUE,
                    PARTITIONS);
            Instant base = Instant.now();
            String partyId = "w20-party";
            List<MatchmakingTicket> party = List.of(
                    ticket("w20-p1", "w20-party-member-1", base, partyId),
                    ticket("w20-p2", "w20-party-member-2", base, partyId),
                    ticket("w20-p3", "w20-party-member-3", base, partyId));
            await(tickets.createPartyTickets(party));

            List<String> keysWithMembers = new ArrayList<>();
            for (String queueKey : RedisKeys.queueKeys(MODE.label(), REGION.name(), PARTITIONS)) {
                if (zcard(redis, queueKey) > 0) {
                    keysWithMembers.add(queueKey);
                }
            }
            assertEquals(1, keysWithMembers.size(),
                    "every member of a party must hash to the same partition: " + keysWithMembers);
            assertEquals(3, zcard(redis, keysWithMembers.get(0)));
        } finally {
            clean(redis);
            redis.close();
            vertx.close();
        }
    }

    @Test
    void queueDepthIsTheSumOfEveryPartition() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            clean(redis);
            TicketRepository tickets = new RedisTicketRepository(redis, RedisTicketRepository.UNBOUNDED_QUEUE,
                    PARTITIONS);
            Instant base = Instant.now();
            for (int i = 0; i < 200; i++) {
                await(tickets.save("w20-depth-" + i, ticket("w20-depth-ticket-" + i, "w20-depth-player-" + i, base,
                        null)));
            }
            assertEquals(200, await(tickets.queueDepth(MODE.label(), REGION.name())),
                    "the depth of a queue is the sum of its partitions, not one partition's size");
        } finally {
            clean(redis);
            redis.close();
            vertx.close();
        }
    }

    @Test
    void partitioningDoesNotRaiseTheAdmissionCap() throws Exception {
        Vertx vertx = Vertx.vertx();
        Redis redis = Redis.createClient(vertx, new RedisOptions().setConnectionString(REDIS_URI));
        try {
            clean(redis);
            TicketRepository tickets = new RedisTicketRepository(redis, CAPACITY, PARTITIONS);
            Instant base = Instant.now();
            int admitted = 0;
            int refused = 0;
            for (int i = 0; i < 200; i++) {
                try {
                    await(tickets.createIdempotent("w20-cap-" + i,
                            ticket("w20-cap-ticket-" + i, "w20-cap-player-" + i, base, null)));
                    admitted++;
                } catch (QueueFullException full) {
                    refused++;
                }
            }
            long depth = await(tickets.queueDepth(MODE.label(), REGION.name()));
            assertEquals(CAPACITY, admitted,
                    "the per-partition cap is floor(C/P), so the sum is exactly C: admitted " + admitted
                            + " refused " + refused + " depth " + depth);
            assertEquals(176, refused);
            assertTrue(depth <= CAPACITY, "partitioning must not raise the cap: total depth " + depth);
        } finally {
            clean(redis);
            redis.close();
            vertx.close();
        }
    }

    /** The first player id whose hash lands in {@code wanted}, so the test never guesses. */
    private static String playerInPartition(int wanted) {
        for (int i = 0; i < 10_000; i++) {
            String player = "w20-partition-player-" + i;
            if (QueuePartitions.partitionOf(player, PARTITIONS) == wanted) {
                return player;
            }
        }
        fail("no player id landed in partition " + wanted);
        return null;
    }

    private static String partitionKeyOf(String playerId) {
        int partition = QueuePartitions.partitionOf(playerId, PARTITIONS);
        return RedisKeys.queueKey(MODE.label(), REGION.name(), partition, PARTITIONS);
    }

    private static MatchmakingTicket ticket(String id, String playerId, Instant queuedAt, String partyId) {
        return new MatchmakingTicket(id, playerId, MODE, REGION, 80, 50, queuedAt, TicketState.SEARCHING, 0L,
                partyId);
    }

    private static <T> T await(io.vertx.core.Future<T> future) throws Exception {
        try {
            return future.toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        } catch (java.util.concurrent.ExecutionException failure) {
            if (failure.getCause() instanceof QueueFullException full) {
                throw full;
            }
            throw failure;
        }
    }

    private static long zcard(Redis redis, String key) throws Exception {
        Response size = redis.send(Request.cmd(Command.ZCARD).arg(key))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        return size != null ? size.toLong() : 0L;
    }

    private static List<String> keys(Redis redis, String pattern) throws Exception {
        Response found = redis.send(Request.cmd(Command.KEYS).arg(pattern))
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        List<String> keys = new ArrayList<>();
        for (int i = 0; found != null && i < found.size(); i++) {
            keys.add(found.get(i).toString());
        }
        return keys;
    }

    private static void clean(Redis redis) throws Exception {
        List<String> toDelete = new ArrayList<>(RedisKeys.queueKeys(MODE.label(), REGION.name(), PARTITIONS));
        toDelete.addAll(keys(redis, "ticket:w20-*"));
        // The idempotency keys outlive the tickets (their own TTL), so a second run without a
        // `flushall` replayed 48 of the 200 creations: the script answered with the stored ticket and
        // the loop counted an admission that never reached the queue (admitted 72, depth 24). The
        // suite must be re-runnable without flushing a shared Redis.
        toDelete.addAll(keys(redis, "idem:w20-*"));
        toDelete.addAll(keys(redis, "match-pending:*"));
        if (toDelete.isEmpty()) {
            return;
        }
        Request del = Request.cmd(Command.DEL);
        for (String key : toDelete) {
            del.arg(key);
        }
        redis.send(del).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }
}
